from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("brand").orderBy("price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category")
w5 = Window.orderBy(F.col("stock_qty").desc(), "id")

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-07-21").cast("date"))
    .select(
        "*",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

scored = (
    filtered
    .withColumn("cost_cume_dist", F.cume_dist().over(w))
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w2))
    .withColumn("last_known_stock_qty_2", F.last("stock_qty", ignorenulls=True).over(w3))
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

eligible = (
    scored
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date")), "left_anti")
)

base = (
    eligible
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-07-06"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "price_filled",
        "brand_clean",
        "cost_cume_dist",
        "last_known_stock_qty_2",
        "seq_no",
        F.add_months("launch_date", 3).alias("launch_date_plus_months"),
    )
)

result = (
    base
    .select(
        "id",
        "category",
        F.col("price_filled").alias("PRICE_FILLED_VAL"),
        F.col("cost").alias("COST_VAL"),
        F.col("price").alias("PRICE_VAL"),
    )
    .unpivot(
        ids=["id", "category"],
        values=["PRICE_FILLED_VAL", "COST_VAL", "PRICE_VAL"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
