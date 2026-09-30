from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("units", "id")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy(F.year("launch_date"), "brand").orderBy("cost", "id")
w_grp = Window.partitionBy(F.month("launch_date")).orderBy("row_seq", "id")
w5 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-3, -1)

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-06-28").cast("date")) & F.col("store_id").isin([2, 4, 5, 6]))
    .withColumn("sale_date_weekofyear", F.weekofyear("sale_date"))
)

with_dims = (
    base
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.units",
        "e.revenue",
        "e.sale_date_weekofyear",
        F.col("d.launch_date"),
        F.col("d.brand"),
        F.col("d.cost"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "revenue",
        "sale_date_weekofyear",
        "launch_date",
        "brand",
        "cost",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

enriched = (
    with_windows
    .filter(F.col("brand") == "Stark")
    .select(
        "id",
        "brand",
        F.lag("cost", 1).over(w3).alias("prev_cost"),
        F.ntile(3).over(w_grp).alias("row_seq_bucket"),
        F.avg("revenue").over(w5).alias("revenue_avg_3_prior"),
    )
)

result = (
    enriched
    .select("id", "brand", "revenue_avg_3_prior")
    .orderBy(F.col("id").asc_nulls_last())
)
