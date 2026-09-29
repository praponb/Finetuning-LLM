from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w_grp = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").desc())
w6 = Window.partitionBy("supplier_id", "category").orderBy(F.col("cost").asc_nulls_first())

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-03-04"))) & F.col("supplier_id").isNotNull())
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "tags",
        "supplier_id",
        "cost_filled",
        "brand_clean",
        F.avg("price").over(w_running).alias("price_avg_2_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("cost", ignorenulls=True).over(w_grp).alias("last_known_cost"),
        F.corr("price", "stock_qty").over(w4).alias("corr_price_stock_qty"),
        F.dense_rank().over(w5).alias("price_dense_rank"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    ranked
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.rating"))
)

wide = (
    with_dims
    .groupBy("supplier_id")
    .pivot("category", ["Books", "Electronics", "Grocery"])
    .agg(F.avg("cost").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select(
        "supplier_id",
        "Books_total",
        "Electronics_total",
        "Electronics_n",
        "Grocery_total",
        "Grocery_n",
    )
    .orderBy(F.col("Electronics_n").asc_nulls_first())
)
