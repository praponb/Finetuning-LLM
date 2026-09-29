from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_by_start_dat = Window.partitionBy(F.year("start_date")).orderBy("start_date", "id")
w_product_group_holder_id = Window.partitionBy("product_group", "holder_id")

filtered = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2026) & (F.col("premium") >= 2319.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("product").isin("LIFE", "TRAVEL"), "group_a").when(F.col("product") == "HEALTH", "group_b").otherwise("other").alias("product_group"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
    )
)

windowed = (
    filtered
    .withColumn("prev2_premium_distance", F.lag("premium_distance", 2, 0).over(w_start_date_year_by_start_dat))
    .withColumn("product_group_holder_id_min_premium_distance", F.min("premium_distance").over(w_product_group_holder_id))
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.max("premium_distance").alias("overall_max_premium_distance")))
    .filter(F.col("premium_distance") < F.col("overall_max_premium_distance"))
    .drop("overall_max_premium_distance")
)

result = (
    with_activity
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "product_group",
        "product_status_label",
        "premium_distance",
        "prev2_premium_distance",
        "product_group_holder_id_min_premium_distance",
        F.concat_ws("|", F.col("product"), F.col("product_status_label")).alias("product_product_status_label_label"),
    )
    .orderBy("product_group", F.col("product").desc(), "id")
    .limit(20)
)
