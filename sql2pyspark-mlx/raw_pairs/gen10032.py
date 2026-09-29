from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("quantity", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-21")))
    .select(
        "*",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.product_name"), F.col("b.brand"))
)

scored = (
    enriched
    .filter(F.col("product_id").isin([3, 5, 6, 7]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock_label",
        "brand",
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("seq_no"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.opened_date"))
)

result = (
    joined
    .rollup(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("seq_no") >= 9, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("next_quantity"))).alias("initech_total_next_quantity"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
    .filter(F.col("initech_total_next_quantity") > 0)
)
