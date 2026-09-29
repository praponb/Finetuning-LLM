from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-12-12").cast("date")) & F.col("discount_pct").between(0.15, 0.32))
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.round((F.col("discount_pct") * 1.07), 2).alias("discount_pct_adjusted"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code", "resolved_channel")
)
