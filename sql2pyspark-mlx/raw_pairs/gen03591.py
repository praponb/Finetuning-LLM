from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "launch_date")).orderBy("price", "id")
w_part = Window.orderBy("stock_qty", "id").rowsBetween(-5, -1)

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-08-03"))) & (F.col("category") == "Electronics"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        (F.col("cost") - F.col("price")).alias("margin"),
    )
)

result = (
    filtered
    .filter(F.col("category") != "Grocery")
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "cost_filled",
        "margin",
        F.lead("price", 2).over(w).alias("next_price"),
        F.avg("margin").over(w_part).alias("margin_avg_5_prior"),
    )
)
