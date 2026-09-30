from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id")
w2 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "entry_price",
        F.lead("entry_price", 2).over(w).alias("next_entry_price"),
        F.first("lowest_price", ignorenulls=True).over(w2).alias("first_known_lowest_price"),
    )
)
