from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy("cost")
w3 = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("launch_date", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.date_trunc("MONTH", "launch_date")).orderBy(F.col("cost").desc_nulls_last())
w6 = Window.partitionBy(F.month("launch_date"), "brand")
w7 = Window.partitionBy("brand").orderBy("is_grocery_avg_6_prior", "id")
w8 = Window.partitionBy("brand").orderBy("cost_rank", "id")
w9 = Window.partitionBy("brand").orderBy(F.col("store_sale_revenue_avg_2_prior").desc(), "id")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-04-04").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("stock_qty").cast("string").alias("stock_qty_num"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.launch_date")), "left_semi")
)

base = (
    not_flagged
    .filter(F.col("supplier_id").isin([8, 31, 33]))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        "stock_qty_num",
        F.when(F.col("category") == "Grocery", 1).otherwise(0).alias("is_grocery"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

with_windows = (
    with_last
    .filter(F.col("store_sale_revenue") < 1800)
    .select(
        "*",
        F.dense_rank().over(w2).alias("cost_dense_rank"),
        F.avg("is_grocery").over(w3).alias("is_grocery_avg_6_prior"),
        F.avg("store_sale_revenue").over(w4).alias("store_sale_revenue_avg_2_prior"),
        F.rank().over(w5).alias("cost_rank"),
        F.count("cost").over(w6).alias("launch_date_month_count_cost"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.lag("store_sale_units").over(w7).alias("prev_store_sale_units"),
        F.lead("store_sale_units").over(w7).alias("next_store_sale_units"),
        F.lead("is_grocery_avg_6_prior").over(w7).alias("next_is_grocery_avg_6_prior"),
        F.row_number().over(w7).alias("row_seq"),
        F.ntile(3).over(w8).alias("cost_rank_bucket"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .groupingSets(
        [["city", "country", "brand"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("p.country").alias("country"),
        F.col("e.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.prev_store_sale_units")).alias("total_prev_store_sale_units"),
        F.round(F.sum(F.col("e.cost_rank")), 2).alias("total_cost_rank"),
        F.grouping_id().alias("grouping_level"),
    )
)
