from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("stock_qty_times_cost").desc(), "id")

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Globex")))
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.concat("category", F.lit(", "), "product_name").alias("category_product_name"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.category",
        "src.cost",
        "src.stock_qty",
        "src.tags",
        "src.supplier_id",
        "src.category_product_name",
        F.col("p.city"),
    )
)

cleaned = (
    joined
    .filter(F.col("cost").between(380, 640))
    .select("*", (F.col("stock_qty") * F.col("cost")).alias("stock_qty_times_cost"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "category",
        "cost",
        "supplier_id",
        "category_product_name",
        "city",
        "stock_qty_times_cost",
    )
)

result = (
    ranked
    .alias("e")
    .join(df_suppliers.alias("dim"), F.col("e.supplier_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["supplier_city", "category"], ["supplier_city"]],
        F.col("dim.city").alias("supplier_city"),
        F.col("e.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.stock_qty_times_cost")).alias("avg_stock_qty_times_cost"),
        F.round(F.avg(F.col("e.cost")), 2).alias("avg_cost"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
