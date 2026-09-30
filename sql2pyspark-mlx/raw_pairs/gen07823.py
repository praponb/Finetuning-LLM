from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

cleaned_inventory = (
    cleaned
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-05-22"))) & F.col("store_id").isin([2, 3, 4]))
    .withColumn("months_since_last_restock", (F.datediff(F.current_date(), "last_restock") / 30))
)

base = (
    cleaned_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-02-21")))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.year("last_restock").alias("last_restock_year"),
    )
)

valid = (
    base
    .filter(F.col("last_restock") >= F.lit("2023-03-01").cast("date"))
    .select(
        "*",
        (F.col("last_restock_year") * F.col("quantity_rounded")).alias("last_restock_year_times_quantity_rounded"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.product_name"))
)

scored = (
    joined
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_rounded",
        "months_since_last_restock",
        "last_restock_year",
        "last_restock_year_times_quantity_rounded",
        "last_restock_month_end",
        "product_name",
        F.sum("months_since_last_restock").over(w).alias("running_sum_months_since_last_restock"),
        F.min("months_since_last_restock").over(w).alias("running_min_months_since_last_restock"),
        F.max("months_since_last_restock").over(w).alias("running_max_months_since_last_restock"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_products.alias("dim"), F.col("a.product_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.last_restock_year")).alias("max_last_restock_year"),
        F.grouping_id().alias("level_id"),
    )
)
