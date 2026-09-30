from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date_minus_43d").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("revenue").asc_nulls_first())
w3 = Window.partitionBy(F.year("sale_date"), "store_id")
w_ordered = Window.orderBy(F.col("corr_units_revenue").desc_nulls_first())
w5 = Window.orderBy("corr_units_revenue")
w_hist = Window.orderBy(F.col("corr_units_revenue").asc_nulls_last())

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-10-03"))) & F.col("revenue").between(1500, 2400))
    .withColumn("sale_date_minus_43d", F.date_sub("sale_date", 43))
)

with_dims = (
    base
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") == "Bangkok")
    .select(
        "src.id",
        "src.store_id",
        "src.sale_date",
        "src.units",
        "src.revenue",
        "src.sale_date_minus_43d",
        F.col("p.sqft"),
    )
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    deduped
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id") == 2)
        .select("id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "sale_date_minus_43d",
        "rn",
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "sale_date_minus_43d",
        F.cume_dist().over(w_ordered).alias("corr_units_revenue_cume_dist"),
        F.percent_rank().over(w5).alias("corr_units_revenue_percent_rank"),
        F.percent_rank().over(w_hist).alias("corr_units_revenue_percent_rank_2"),
    )
)

report = enriched.select("id", "corr_units_revenue_percent_rank")

result = (
    report
    .select("id", "corr_units_revenue_percent_rank")
    .orderBy("corr_units_revenue_percent_rank", "id")
)
