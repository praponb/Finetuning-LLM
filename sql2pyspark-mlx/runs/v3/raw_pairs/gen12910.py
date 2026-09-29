from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2023-06-01") & F.col("status").isin(["RETURNED", "LOST"]))
    .groupBy(
        F.month("shipped_date").alias("shipped_month"),
        "carrier",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.count("*").alias("row_count"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)
