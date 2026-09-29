from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("product_cost_2", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-07-23")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.round((F.col("units") * 1.15), 2).alias("adjusted_units"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.adjusted_units",
        F.col("b.cost"),
        F.col("b.launch_date"),
        F.col("b.brand"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.supplier_id") == 39)
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.cost",
        "src.launch_date",
        F.col("b.launch_date").alias("product_launch_date"),
        F.col("b.cost").alias("product_cost"),
        F.col("b.brand").alias("product_brand"),
    )
)

joined_store_sales = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.launch_date",
        "s.product_cost",
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.cost").alias("product_cost_2"),
    )
)

with_windows = (
    joined_store_sales
    .select(
        "id",
        "product_id",
        "price",
        "product_cost_2",
        F.row_number().over(w).alias("event_seq"),
        F.avg("product_cost_2").over(w2).alias("product_cost_2_avg_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("product_id").isin([5, 6]))
    .select(
        "*",
        F.round("product_cost_2_avg_2", 0).alias("product_cost_2_avg_2_rounded"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "product_id",
        F.col("product_cost_2_avg_2_rounded").alias("PRODUCT_COST_2_AVG_2_ROUNDED"),
        "product_cost_2",
        F.col("price").alias("PRICE_VAL"),
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["PRODUCT_COST_2_AVG_2_ROUNDED", "product_cost_2", "PRICE_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
