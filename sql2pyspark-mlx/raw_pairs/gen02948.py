from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w_grp = Window.orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first())
w_part = Window.partitionBy("brand", "product_id").orderBy("running_next_quantity", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("running_next_quantity").desc_nulls_first())
w7 = Window.partitionBy("brand", "order_id").orderBy("cost", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("product_id").orderBy("cost_avg_3", "id")

with_windows = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
)

with_windows_order_items = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "quantity_share",
        F.first("next_quantity", ignorenulls=True).over(w_grp).alias("first_known_next_quantity"),
        F.percent_rank().over(w4).alias("quantity_percent_rank"),
        F.sum("next_quantity").over(w_grp).alias("running_next_quantity"),
    )
)

enriched = (
    with_windows_order_items
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id").eqNullSafe(F.col("d.id")), "inner")
    .select("e.*", F.col("d.cost"), F.col("d.stock_qty"), F.col("d.brand"))
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "quantity_share",
        "first_known_next_quantity",
        "running_next_quantity",
        "brand",
        F.avg("cost").over(w_part).alias("cost_avg_3"),
        F.percent_rank().over(w6).alias("running_next_quantity_percent_rank"),
        F.avg("running_next_quantity").over(w7).alias("running_next_quantity_avg_6_prior"),
    )
)

deduped = (
    enriched_order_items
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

enriched_order_items_2 = (
    deduped
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.brand",
        "e.cost_avg_3",
        "e.running_next_quantity_percent_rank",
        F.col("b.brand").alias("product_brand"),
        F.col("b.stock_qty"),
    )
)

result = (
    enriched_order_items_2
    .groupBy(
        F.when(F.col("cost_avg_3") >= 765, "LARGE").otherwise("SMALL").alias("cost_avg_3_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("cost_avg_3"), 2).alias("avg_cost_avg_3"),
        F.round(F.sum("cost_avg_3") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_avg_3_per_acme"),
    )
    .filter(F.col("distinct_product_ids") >= 1)
    .orderBy(F.col("cost_avg_3_tier").desc_nulls_last())
)
