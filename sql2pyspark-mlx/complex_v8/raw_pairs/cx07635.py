from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("premium") < 3521.8)
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

result = (
    scored
    .groupBy("end_date")
    .pivot("status", ["ACTIVE", "CANCELLED", "EXPIRED", "LAPSED"])
    .agg(F.max("premium"))
    .orderBy("end_date")
)
