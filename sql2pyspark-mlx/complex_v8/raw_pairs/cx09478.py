from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("premium") > 2111.3))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

per_group = (
    prepared_policys
    .groupBy("product", "status")
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("n_status_active"),
        F.count("*").alias("row_count"),
        F.max("end_date").alias("last_end_date"),
    )
)

annotated = (
    per_group
    .select(
        "product",
        "status",
        "n_status_active",
        "row_count",
        "last_end_date",
        F.date_trunc("month", F.col("last_end_date")).alias("last_end_date_month"),
        F.abs(F.col("n_status_active") - 5).alias("n_status_active_distance"),
    )
)

result = (
    annotated
    .groupBy("row_count")
    .pivot("product", ["AUTO", "HEALTH", "HOME", "LIFE"])
    .agg(F.sum("n_status_active_distance").alias("amount"), F.count("n_status_active_distance").alias("n"))
)
