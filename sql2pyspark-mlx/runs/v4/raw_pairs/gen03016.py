from pyspark.sql import functions as F

result = (
    df_promotions
    .select("channel")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") >= 0.18)
        .select("channel")
    )
)
