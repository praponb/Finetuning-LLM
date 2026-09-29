from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_plus_months", "id")
w3 = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-02-12").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 63).alias("last_restock_plus_63d"),
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.lag("quantity", 1, 0).over(w2).alias("prev_quantity_2"),
        F.avg("quantity").over(w3).alias("quantity_avg_3"),
    )
)

valid = (
    enriched
    .filter(F.col("prev_quantity_2") != 238)
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

grouped = (
    valid
    .groupBy(
        F.date_trunc("YEAR", "last_restock_plus_63d").alias("year_start"),
        F.date_trunc("WEEK", "last_restock_plus_months").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    grouped
    .select("year_start", "week_start", "distinct_ids", "quantity_p50")
    .orderBy(F.col("year_start").asc_nulls_last())
)
