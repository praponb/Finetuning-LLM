from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("days_since_sale_date_whole", "id")

filtered = (
    df_store_sales
    .filter(F.col("revenue") >= 2400)
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

valid = (
    filtered
    .filter(F.col("revenue").isNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "days_since_sale_date",
        F.ceil("days_since_sale_date").alias("days_since_sale_date_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.last("days_since_sale_date", ignorenulls=True).over(w).alias("last_known_days_since_sale_date"),
        F.max("days_since_sale_date_whole").over(w2).alias("product_id_max_days_since_sale_date_whole"),
        F.lag("units", 1).over(w3).alias("prev_units"),
    )
)

matched_keys = (
    scored
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([2, 8, 10]))
        .select("product_id", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "product_id",
        "revenue",
        "days_since_sale_date_whole",
        "product_id_max_days_since_sale_date_whole",
        "prev_units",
    )
    .orderBy(F.col("prev_units").desc_nulls_first(), "product_id")
)
