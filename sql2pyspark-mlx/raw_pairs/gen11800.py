from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("adjusted_quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "category").orderBy(F.col("quantity_rounded_minus_adjusted_quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity_rounded", "id")
w4 = Window.partitionBy("category").orderBy("quantity_rounded_minus_quantity_rounded_minus_adjusted_quantity", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("category").orderBy("quantity_rounded", "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-07").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "quantity_rounded", "adjusted_quantity")
)

filtered_inventory = (
    ranked
    .filter(F.col("product_id").isin([3, 6, 8]))
    .withColumn("quantity_rounded_minus_adjusted_quantity", (F.col("quantity_rounded") - F.col("adjusted_quantity")))
)

filtered_inventory_2 = (
    filtered_inventory
    .filter(F.col("quantity_rounded_minus_adjusted_quantity") > 28)
    .withColumn("quantity_rounded_filled", F.coalesce("quantity_rounded", F.lit(0)))
    .withColumn("quantity_rounded_minus_quantity_rounded_minus_adjusted_quantity", (F.col("quantity_rounded") - F.col("quantity_rounded_minus_adjusted_quantity")))
)

enriched = (
    filtered_inventory_2
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.category"), F.col("b.stock_qty"), F.col("b.product_name"))
)

metrics = (
    enriched
    .select(
        "id",
        "quantity_rounded",
        "adjusted_quantity",
        "quantity_rounded_filled",
        "category",
        F.row_number().over(w2).alias("row_seq"),
        F.lead("quantity_rounded_minus_adjusted_quantity", 1, 0).over(w3).alias("next_quantity_rounded_minus_adjusted_quantity"),
        F.avg("quantity_rounded").over(w4).alias("quantity_rounded_avg_2"),
        F.lead("adjusted_quantity", 1, 0).over(w5).alias("next_adjusted_quantity"),
    )
)

result = (
    metrics
    .select(
        "id",
        "quantity_rounded",
        "adjusted_quantity",
        "quantity_rounded_filled",
        "category",
        "row_seq",
        "next_quantity_rounded_minus_adjusted_quantity",
        "quantity_rounded_avg_2",
        F.when(F.col("category").rlike("^(Grocery|Fashion)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("category_zone").desc_nulls_first())
)
