from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("email") > 11) & F.col("segment").isin(["SMB", "Corporate", "Consumer"]))
    .select(
        "id",
        "segment",
        "credit_limit",
        "country",
        F.substring("segment", 1, 3).alias("segment_prefix"),
    )
    .orderBy(F.col("id").desc(), "country")
    .limit(5)
)
