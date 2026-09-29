from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("prev_adjusted_revenue_int").desc_nulls_last())

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-16").cast("date"))
    .select(
        "*",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.round((F.col("revenue") * 100), 2).alias("revenue_adjusted"),
    )
)

base_store_sales = (
    base
    .filter((F.col("sale_date") >= F.lit("2023-07-27").cast("date")) & (F.year("sale_date") >= 2023))
    .select(
        "*",
        F.round((F.col("revenue_int") * 1.1), 2).alias("adjusted_revenue_int"),
        F.when(F.col("revenue_int") >= 69, "High").when(F.col("revenue_int") >= 25, "Medium").otherwise("Low").alias("revenue_int_band"),
    )
)

with_windows = (
    base_store_sales
    .filter(F.col("store_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_int",
        "revenue_adjusted",
        F.lag("revenue").over(w_recent).alias("prev_revenue"),
        F.lead("revenue").over(w_recent).alias("next_revenue"),
        F.lag("adjusted_revenue_int").over(w_recent).alias("prev_adjusted_revenue_int"),
        F.lead("adjusted_revenue_int").over(w_recent).alias("next_adjusted_revenue_int"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.ntile(4).over(w3).alias("units_bucket"),
        F.percent_rank().over(w4).alias("prev_adjusted_revenue_int_percent_rank"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.brand"), F.col("p.launch_date"))
)

result = (
    with_dims
    .groupBy("product_id")
    .pivot("brand", ["Acme", "Umbrella", "Initech"])
    .agg(
        F.max("next_adjusted_revenue_int").alias("total"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "product_id",
        F.col("Acme_total").alias("acme_total"),
        F.col("Acme_cnt").alias("acme_cnt"),
        F.col("Umbrella_total").alias("umbrella_total"),
        F.col("Umbrella_cnt").alias("umbrella_cnt"),
        F.col("Initech_total").alias("initech_total"),
        F.col("Initech_cnt").alias("initech_cnt"),
    )
)
