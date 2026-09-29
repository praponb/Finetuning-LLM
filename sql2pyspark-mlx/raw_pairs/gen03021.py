from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())
w4 = Window.orderBy(F.col("units_whole").desc())
w5 = Window.partitionBy("product_id").orderBy("units_whole")
w6 = Window.partitionBy(F.date_trunc("month", "sale_date"), "product_id").orderBy("units_whole")
w7 = Window.orderBy(F.col("units_whole_rank_2").desc_nulls_first())

latest = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    latest
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") >= 26)
        .select("id", "product_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-04-15"))) & F.col("product_id").isin([4, 7]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "rn",
        F.greatest("rn", "revenue").alias("greatest_rn_revenue"),
        F.ceil("units").alias("units_whole"),
    )
)

filtered = (
    base
    .filter((F.col("sale_date") >= F.lit("2023-10-05").cast("date")) & F.col("units").between(25, 26))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "rn",
        "units_whole",
        F.when(F.col("units_whole") >= 62, "High").when(F.col("units_whole") >= 26, "Medium").otherwise("Low").alias("units_whole_tier"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "product_id",
        "rn",
        "units_whole_tier",
        F.percent_rank().over(w2).alias("revenue_percent_rank"),
        F.rank().over(w_ordered).alias("revenue_rank"),
        F.rank().over(w4).alias("units_whole_rank"),
        F.rank().over(w5).alias("units_whole_rank_2"),
        F.rank().over(w6).alias("units_whole_rank_3"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select(
        "s.product_id",
        "s.units_whole_tier",
        "s.revenue_rank",
        "s.units_whole_rank_2",
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
)

result = (
    with_dims
    .filter(F.col("product_id").isin([4, 10]))
    .select(
        "product_id",
        "revenue_rank",
        "units_whole_rank_2",
        F.dense_rank().over(w7).alias("units_whole_rank_2_dense_rank"),
    )
)
