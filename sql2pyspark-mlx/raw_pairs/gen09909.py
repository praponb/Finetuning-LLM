from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("quantity", "id")
w_ordered = Window.orderBy(F.col("quantity_avg_3").desc())
w5 = Window.orderBy("row_seq")
w6 = Window.partitionBy("product_id").orderBy(F.col("quantity_avg_3").desc())
w7 = Window.partitionBy("product_id").orderBy(F.col("row_seq").desc_nulls_last())
w8 = Window.partitionBy("product_id")

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "product_id", "quantity")
)

enriched = (
    deduped
    .select(
        "id",
        "product_id",
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_order_items = (
    enriched
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .filter(F.col("d.price") <= 1170)
    .select("s.*", F.col("d.stock_qty"))
)

remaining_keys = (
    enriched_order_items
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 20)
        .select("product_id")
    )
)

verified = (
    enriched_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "product_id",
        "quantity_avg_3",
        "row_seq",
        F.rank().over(w_ordered).alias("quantity_avg_3_rank"),
        F.cume_dist().over(w5).alias("row_seq_cume_dist"),
        F.rank().over(w6).alias("quantity_avg_3_rank_2"),
        F.percent_rank().over(w7).alias("row_seq_percent_rank"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w8)), 4).alias("row_seq_share"),
    )
)

result = (
    scored
    .select("product_id", "row_seq_percent_rank", "row_seq_cume_dist")
    .unpivot(
        ids=["product_id"],
        values=["row_seq_percent_rank", "row_seq_cume_dist"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
