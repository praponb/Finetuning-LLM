from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_recent = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("rn", "id").rowsBetween(-6, -1)

latest = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    latest
    .filter(F.col("product_id").isin([3, 4, 5, 8]))
    .withColumn("quantity_times_rn", (F.col("quantity") * F.col("rn")))
)

metrics = (
    cleaned
    .withColumn("prev_quantity_times_rn", F.lag("quantity_times_rn").over(w_recent))
    .withColumn("next_quantity_times_rn", F.lead("quantity_times_rn").over(w_recent))
    .withColumn("prev_quantity", F.lag("quantity").over(w_recent))
    .withColumn("next_quantity", F.lead("quantity").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w3))
)

enriched = (
    metrics
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.opened_date"), F.col("d.sqft"))
)

joined = (
    enriched
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .filter(F.col("b.stock_qty") < 157)
    .select("e.*", F.col("b.brand"), F.col("b.stock_qty"))
)

result = (
    joined
    .select(
        "id",
        "quantity",
        "last_restock",
        "rn",
        "prev_quantity",
        "next_quantity",
        "opened_date",
        "sqft",
        "brand",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
