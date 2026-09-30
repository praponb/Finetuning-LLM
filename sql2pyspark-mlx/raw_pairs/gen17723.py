from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .select(
        "id",
        "currency",
        "lowest_price",
        F.lag("lowest_price").over(w).alias("prev_lowest_price"),
        F.lead("lowest_price").over(w).alias("next_lowest_price"),
        F.row_number().over(w).alias("event_seq"),
    )
)
