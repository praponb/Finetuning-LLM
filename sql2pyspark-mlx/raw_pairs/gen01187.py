from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy(F.month("launch_date"), "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("supplier_id_sum_rating", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("brand")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-04-04"))) & (F.col("cost") >= 640))
    .select(
        "*",
        F.trim("brand").alias("clean_brand"),
        F.lower("brand").alias("brand_lower"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.supplier_name"), F.col("s.city"), F.col("s.rating"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.sum("rating").over(w).alias("supplier_id_sum_rating"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
    )
)

metrics_products = (
    metrics
    .select(
        "*",
        F.last("supplier_id_sum_rating", ignorenulls=True).over(w2).alias("last_known_supplier_id_sum_rating"),
        F.avg("price_share").over(w3).alias("price_share_avg_3"),
        F.avg("cost").over(w_grp).alias("cost_avg_6"),
        F.avg("price").over(w5).alias("price_avg_3"),
        F.round((F.col("price") / F.sum("price").over(w6)), 4).alias("price_share_2"),
    )
)

base = (
    metrics_products
    .filter((F.col("launch_date") > F.lit("2023-03-06").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_name",
        "city",
        "rating",
        "price_share",
        "last_known_supplier_id_sum_rating",
        "price_share_avg_3",
        "cost_avg_6",
        "price_avg_3",
        "price_share_2",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.col("stock_qty").cast("double").alias("stock_qty_num"),
    )
)

result = (
    base
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "tags",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("supplier_name").rlike("^(EuroGoods GmbH|Tokyo Traders)$"), "EXTENDED").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
)
