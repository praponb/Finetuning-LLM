from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("segment") == "SMB") | F.col("country").isin(["DE", "TH", "JP"]))
)

customers_stats = (
    src
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)

result = (
    customers_stats
    .filter(F.col("pct_new_york") > 24)
    .orderBy(F.col("country").desc())
)
