from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id = Window.partitionBy("status", "holder_id")
w_all_by_row_count = Window.orderBy("row_count", "holder_id", "status")

filtered_policys = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2024)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

grouped = (
    filtered_policys
    .filter(F.col("premium") >= 2013.8)
    .groupBy("holder_id", "status")
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
    )
)

sequenced = (
    grouped
    .withColumn("median_premium_share_of_status_holder_id", F.round(F.col("median_premium") / F.nullif(F.sum("median_premium").over(w_status_holder_id), F.lit(0)), 4))
    .withColumn("row_count_dense_rank", F.dense_rank().over(w_all_by_row_count))
)

result = (
    sequenced
    .select(
        "holder_id",
        "status",
        "row_count",
        "median_premium_share_of_status_holder_id",
        F.least(F.col("row_count"), F.col("row_count_dense_rank")).alias("least_row_count_row_count_dense_rank"),
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
    )
    .orderBy(F.col("holder_id").desc())
)
