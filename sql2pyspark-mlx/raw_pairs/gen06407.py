from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w4 = Window.partitionBy("product_id").orderBy("running_quantity", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-08-17").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

scored = (
    valid
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_filled",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.lead("quantity_filled", 2).over(w2).alias("next_quantity_filled"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

ranked_inventory = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked_inventory
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.brand"), F.col("b.price"), F.col("b.product_name"))
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        F.col("next_quantity_filled").alias("NEXT_QUANTITY_FILLED"),
        F.col("quantity_filled").alias("QUANTITY_FILLED"),
        "price",
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["NEXT_QUANTITY_FILLED", "QUANTITY_FILLED", "price"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
