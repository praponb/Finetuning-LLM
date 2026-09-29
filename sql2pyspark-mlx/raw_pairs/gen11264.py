from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("greatest_unit_price_quantity", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("prev_unit_price", "id")
w_hist = Window.partitionBy("order_id").orderBy("unit_price", "id")
w5 = Window.partitionBy("order_id").orderBy("unit_price_minus_quantity_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last_known_unit_price", "id")

base = (
    df_order_items
    .filter(F.col("product_id") == 16)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_adjusted"),
    )
)

metrics = (
    base
    .select(
        "id",
        "order_id",
        "unit_price",
        "unit_price_adjusted",
        F.lag("unit_price", 1).over(w).alias("prev_unit_price"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    ranked
    .filter(F.col("order_id").isin([3, 17, 35]))
    .select(
        "id",
        "order_id",
        "unit_price",
        "unit_price_adjusted",
        "prev_unit_price",
        "quantity_cume_dist",
        F.when(F.col("prev_unit_price") >= 420, "High").when(F.col("prev_unit_price") >= 170, "Medium").otherwise("Low").alias("prev_unit_price_band"),
    )
)

cleaned_order_items = (
    cleaned
    .filter(F.col("prev_unit_price") <= 390)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_cume_dist",
        (F.col("unit_price") - F.col("quantity_cume_dist")).alias("unit_price_minus_quantity_cume_dist"),
    )
)

scored = (
    cleaned_order_items
    .filter(F.col("quantity_cume_dist") <= 0.41)
    .select(
        "id",
        "quantity_cume_dist",
        "unit_price_minus_quantity_cume_dist",
        F.lead("unit_price", 2, 0).over(w_hist).alias("next_unit_price"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.last("unit_price", ignorenulls=True).over(w5).alias("last_known_unit_price"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "quantity_cume_dist",
        "next_unit_price",
        "row_seq",
        "last_known_unit_price",
        F.lag("row_seq").over(w6).alias("prev_row_seq"),
        F.lead("unit_price_minus_quantity_cume_dist").over(w6).alias("next_unit_price_minus_quantity_cume_dist"),
        F.lag("unit_price_minus_quantity_cume_dist").over(w6).alias("prev_unit_price_minus_quantity_cume_dist"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_unit_price_minus_quantity_cume_dist") >= 67, "LARGE").otherwise("SMALL").alias("prev_unit_price_minus_quantity_cume_dist_tier"),
        F.when(F.col("next_unit_price") >= 390, "LARGE").otherwise("SMALL").alias("next_unit_price_tier"),
    )
    .agg(
        F.round(F.avg("next_unit_price_minus_quantity_cume_dist"), 2).alias("avg_next_unit_price_minus_quantity_cume_dist"),
        F.round(F.avg("last_known_unit_price"), 2).alias("avg_last_known_unit_price"),
        F.sum("last_known_unit_price").alias("total_last_known_unit_price"),
    )
)
