from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("entry_price", "id")
w3 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(-3, -1)
w_recent = Window.partitionBy("plan").orderBy(F.col("entry_price").desc(), "id")
w5 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "plan",
        "entry_price",
        F.avg("entry_price").over(w_part).alias("plan_avg_entry_price"),
        F.lag("entry_price", 1).over(w2).alias("prev_entry_price"),
        F.avg("entry_price").over(w3).alias("entry_price_avg_3_prior"),
        F.ntile(3).over(w_recent).alias("entry_price_bucket"),
        F.first("entry_price", ignorenulls=True).over(w5).alias("first_known_entry_price"),
    )
)
