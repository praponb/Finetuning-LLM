from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") <= "2025-07-12") & ((F.col("shipped_date") <= "2023-10-01") | (F.col("delivered_date") < "2025-07-24")))
    .select(
        "shipped_date",
        F.round((F.col("cost") * 100), 2).alias("cost_adjusted"),
        F.when(F.col("cost") >= 104, "High").when(F.col("cost") >= 47, "Medium").otherwise("Low").alias("cost_level"),
    )
    .orderBy(F.col("shipped_date").desc())
)
