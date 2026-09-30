from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.year("signup_date") == 2023) & ~F.col("segment").isin(["SMB", "Consumer"]))
)

agg = (
    recent_customers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
)

result = agg
