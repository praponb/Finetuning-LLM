from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Kerry") & F.col("status").isin(["RETURNED", "LOST"]) & (F.col("shipped_date") <= "2023-09-03"))
    .rollup(
        F.month("shipped_date").alias("shipped_month"),
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(F.countDistinct("cost").alias("unique_orders"))
    .orderBy("shipped_month", F.col("shipped_cohort").desc())
)
