from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-11-01", "2025-09-01"))
    .groupBy(
        "carrier",
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.count("cost").alias("cost_count"),
    )
)
