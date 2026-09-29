from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-07-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 51).alias("last_restock_plus_51d"),
        (F.datediff(F.current_date(), "last_restock") / 365).alias("years_since_last_restock"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "years_since_last_restock")
)

base = (
    latest
    .filter(F.col("store_id") == 4)
    .select(
        "*",
        F.ceil("years_since_last_restock").alias("years_since_last_restock_whole"),
    )
)

cleaned = (
    base
    .filter(F.col("store_id").isin([1, 6]))
    .select(
        "*",
        F.greatest("years_since_last_restock", "years_since_last_restock_whole").alias("greatest_years_since_last_restock_years_since_last_restock_whole"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "greatest_years_since_last_restock_years_since_last_restock_whole",
        "years_since_last_restock",
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["greatest_years_since_last_restock_years_since_last_restock_whole", "years_since_last_restock"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
