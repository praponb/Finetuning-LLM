from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.month("launch_date")).orderBy("cost", "id")
w3 = Window.partitionBy("product_id", "product_category")
w4 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "category").orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("product_id", "product_category").orderBy(F.col("unit_price").desc_nulls_first())
w6 = Window.orderBy("unit_price", "id")
w7 = Window.partitionBy("category").orderBy("cost", "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("unit_price_avg_5").desc(), "id")
w9 = Window.partitionBy("product_category")
w10 = Window.partitionBy("product_id").orderBy("unit_price", "id")

ranked = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "product_id", "unit_price")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.id", "a.product_id", "a.unit_price", F.col("p.category"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.unit_price",
        "s.category",
        F.col("b.cost"),
        F.col("b.category").alias("product_category"),
        F.col("b.launch_date"),
    )
)

scored = (
    joined
    .filter(F.col("category") == "Sports")
    .withColumn("cost_bucket", F.ntile(3).over(w2))
    .withColumn("product_id_stddev_samp_cost", F.stddev_samp("cost").over(w3))
    .withColumn("unit_price_avg_5", F.avg("unit_price").over(w4))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w5))
    .withColumn("next_unit_price", F.lead("unit_price", 1, 0).over(w6))
)

metrics = (
    scored
    .select(
        "id",
        "product_id",
        "unit_price",
        "cost_bucket",
        "product_id_stddev_samp_cost",
        "unit_price_avg_5",
        "unit_price_dense_rank",
        "next_unit_price",
        F.lead("cost", 1).over(w7).alias("next_cost"),
        F.row_number().over(w8).alias("event_seq"),
        F.round((F.col("next_unit_price") / F.sum("next_unit_price").over(w9)), 4).alias("next_unit_price_share"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "product_id",
        F.col("next_cost").alias("NEXT_COST"),
        F.col("next_unit_price_share").alias("NEXT_UNIT_PRICE_SHARE_VAL"),
        F.col("unit_price").alias("UNIT_PRICE"),
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["NEXT_COST", "NEXT_UNIT_PRICE_SHARE_VAL", "UNIT_PRICE"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
