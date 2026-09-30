from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("cost", "id")
w3 = Window.partitionBy("brand").orderBy(F.col("stock_qty_per_price").desc_nulls_first())
w4 = Window.partitionBy(F.month("launch_date"), "brand").orderBy("launch_date_period", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.to_date("launch_date_period"), "category")
w6 = Window.partitionBy("category").orderBy("cost_avg_2_prior", "id")
w7 = Window.partitionBy("category").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.date_trunc("month", "launch_date_period"), "category").orderBy("stock_qty_per_price", "id")
w9 = Window.partitionBy("category").orderBy("launch_date", "id")
w10 = Window.partitionBy("category").orderBy(F.col("prev_cost_avg_2_prior").desc(), "id")

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-07-27"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.round((F.col("stock_qty") / F.col("price")), 2).alias("stock_qty_per_price"),
        F.date_trunc("WEEK", "launch_date").alias("launch_date_period"),
    )
)

scored = (
    filtered
    .filter(F.col("brand") == "Initech")
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "stock_qty_per_price",
        "launch_date_period",
        F.avg("stock_qty_per_price").over(w).alias("stock_qty_per_price_avg_7d"),
        F.lag("price", 1).over(w2).alias("prev_price"),
        F.rank().over(w3).alias("stock_qty_per_price_rank"),
        F.avg("cost").over(w4).alias("cost_avg_2_prior"),
    )
)

valid = (
    scored
    .filter(F.col("category") == "Electronics")
    .withColumn("stock_qty_per_price_rank_adjusted", F.round((F.col("stock_qty_per_price_rank") * 100), 2))
    .withColumn("cost_rounded", F.round("cost", 1))
)

scored_products = (
    valid
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "stock_qty_per_price",
        "launch_date_period",
        "stock_qty_per_price_avg_7d",
        "prev_price",
        "stock_qty_per_price_rank",
        F.round((F.col("price") / F.sum("price").over(w5)), 4).alias("price_share"),
        F.lag("cost_avg_2_prior", 1, 0).over(w6).alias("prev_cost_avg_2_prior"),
        F.sum("cost_avg_2_prior").over(w7).alias("cumulative_cost_avg_2_prior"),
        F.lead("stock_qty", 1, 0).over(w8).alias("next_stock_qty"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

ranked = (
    scored_products
    .withColumn("rank_in_group", F.row_number().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "stock_qty_per_price",
        "launch_date_period",
        "stock_qty_per_price_rank",
        "prev_cost_avg_2_prior",
        "next_stock_qty",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
