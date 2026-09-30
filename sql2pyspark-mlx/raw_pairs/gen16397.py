from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_scaled_rounded").desc_nulls_first(), "id")
w2 = Window.partitionBy("brand").orderBy("quantity_scaled_rounded", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.orderBy("quantity_scaled_rounded", "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("next_quantity_scaled_rounded").desc(), "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("prev_quantity_scaled_rounded").desc(), "id")
w7 = Window.orderBy(F.col("next_quantity_scaled_rounded").desc(), "id")
w8 = Window.orderBy(F.col("prev_quantity_scaled_rounded").desc_nulls_first())

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-06")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
    )
)

filtered = (
    base
    .filter(F.col("quantity") == 86)
    .withColumn("quantity_scaled_rounded", F.round("quantity_scaled", 0))
    .withColumn("quantity_scaled_minus_quantity", (F.col("quantity_scaled") - F.col("quantity")))
)

enriched = (
    filtered
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity_scaled_rounded",
        F.col("b.category"),
        F.col("b.brand"),
        F.col("b.product_name"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .filter(F.col("rn") > 1)
    .withColumn("prev_quantity_scaled_rounded", F.lag("quantity_scaled_rounded", 1, 0).over(w2))
    .withColumn("product_id_sum_quantity_scaled_rounded", F.sum("quantity_scaled_rounded").over(w3))
    .withColumn("next_quantity_scaled_rounded", F.lead("quantity_scaled_rounded", 1).over(w4))
)

enriched_inventory = (
    scored
    .select(
        "id",
        "store_id",
        "quantity_scaled_rounded",
        "category",
        "brand",
        "prev_quantity_scaled_rounded",
        "product_id_sum_quantity_scaled_rounded",
        F.ntile(4).over(w5).alias("next_quantity_scaled_rounded_bucket"),
        F.ntile(5).over(w6).alias("prev_quantity_scaled_rounded_bucket"),
        F.ntile(3).over(w7).alias("next_quantity_scaled_rounded_bucket_2"),
        F.rank().over(w8).alias("prev_quantity_scaled_rounded_rank"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "store_id",
        F.col("prev_quantity_scaled_rounded").alias("PREV_QUANTITY_SCALED_ROUNDED"),
        F.col("quantity_scaled_rounded").alias("QUANTITY_SCALED_ROUNDED"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["PREV_QUANTITY_SCALED_ROUNDED", "QUANTITY_SCALED_ROUNDED"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
