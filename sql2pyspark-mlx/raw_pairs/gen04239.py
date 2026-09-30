from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy("lowest_price", "id")
w2 = Window.partitionBy("currency", "plan").orderBy("lowest_price", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("plan", "currency").orderBy("lowest_price", "id")
w_ordered = Window.partitionBy("plan", "currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("currency").orderBy(F.col("lowest_price").asc_nulls_last())

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "currency",
        F.lag("lowest_price", 1).over(w_hist).alias("prev_lowest_price"),
        F.avg("lowest_price").over(w2).alias("lowest_price_avg_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.last("lowest_price", ignorenulls=True).over(w_ordered).alias("last_known_lowest_price"),
        F.percent_rank().over(w5).alias("lowest_price_percent_rank"),
    )
)
