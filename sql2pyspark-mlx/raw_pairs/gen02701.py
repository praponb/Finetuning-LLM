from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("entry_price").desc(), "id")

parsed = (
    df_price_tiers
    .filter(F.col("effective_from").between("2024-05-01", "2025-01-01"))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "effective_from",
        F.ntile(4).over(w).alias("entry_price_bucket"),
        F.ntile(4).over(w).alias("entry_price_bucket_2"),
        F.row_number().over(w).alias("row_seq"),
    )
)
