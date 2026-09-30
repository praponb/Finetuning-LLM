from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year = Window.partitionBy(F.year("start_date"))
w_start_date_year_by_days_end = Window.partitionBy(F.year("start_date")).orderBy("days_end_date_to_start_date", "id")
w_start_date_year_by_days_end_2 = Window.partitionBy(F.year("start_date")).orderBy("days_end_date_to_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_year_by_days_end_3 = Window.partitionBy(F.year("start_date")).orderBy("days_end_date_to_start_date")
w_start_date_year_by_days_end_4 = Window.partitionBy(F.year("start_date")).orderBy("days_end_date_to_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("holder_id", "product_group").orderBy("days_end_date_to_start_date_vs_start_date_year_a", "id")

valid_policys = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "LAPSED")) | (F.col("product") == "AUTO"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.when(F.col("product").isin("LIFE", "HOME"), "group_a").when(F.col("product") == "HEALTH", "group_b").otherwise("other").alias("product_group"),
    )
)

windowed = (
    valid_policys
    .select(
        "*",
        F.round(F.col("days_end_date_to_start_date") - F.avg("days_end_date_to_start_date").over(w_start_date_year), 2).alias("days_end_date_to_start_date_vs_start_date_year_a"),
        F.lag("days_end_date_to_start_date", 3).over(w_start_date_year_by_days_end).alias("prev3_days_end_date_to_start_date"),
        F.sum("days_end_date_to_start_date").over(w_start_date_year_by_days_end_2).alias("running_days_end_date_to_start_date"),
        F.round(F.cume_dist().over(w_start_date_year_by_days_end_3), 3).alias("days_end_date_to_start_date_cume_dist"),
        F.first("end_date").over(w_start_date_year_by_days_end_4).alias("first_end_date"),
    )
)

latest = (
    windowed
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy("first_end_date")
    .pivot("product_group", ["group_a", "other", "group_b"])
    .agg(F.sum("prev3_days_end_date_to_start_date"))
)
