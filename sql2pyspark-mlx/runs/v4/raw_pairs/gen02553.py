from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct").between(0.22, 0.26))
        .select("id", "channel")
    )
)
