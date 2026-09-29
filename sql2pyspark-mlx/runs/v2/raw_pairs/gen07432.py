from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .filter(F.col("pct_bangkok") >= 67)
    .select("city", "country", "pct_bangkok")
    .orderBy(F.col("city").desc())
)
