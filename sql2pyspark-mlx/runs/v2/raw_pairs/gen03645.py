from pyspark.sql import functions as F

src = df_customers.filter(F.col("is_active"))

customers_stats = (
    src
    .groupBy("is_active")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
)

result = (
    customers_stats
    .select("is_active", "pct_chiang_mai")
    .orderBy(F.col("pct_chiang_mai").desc(), "is_active")
    .limit(20)
)
