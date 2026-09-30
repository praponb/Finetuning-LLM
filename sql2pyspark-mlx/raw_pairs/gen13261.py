from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("revenue_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.sale_date",
        "s.revenue",
        F.col("p.city"),
        F.col("p.sqft"),
        F.col("p.opened_date"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "store_id",
        "sale_date",
        "sqft",
        "opened_date",
        F.avg("revenue").over(w).alias("revenue_avg_6_prior"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)

base = (
    metrics
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-04-09")))
    .withColumn("sqft_share_filled", F.coalesce("sqft_share", F.lit(0)))
)

scored = (
    base
    .select(
        "*",
        F.avg("sqft_share").over(w3).alias("running_avg_sqft_share"),
        F.max("sqft_share").over(w3).alias("running_max_sqft_share"),
        F.sum("sqft_share").over(w3).alias("running_sum_sqft_share"),
    )
)

result = (
    scored
    .select("id", "store_id", "running_sum_sqft_share", "running_avg_sqft_share")
    .unpivot(
        ids=["id", "store_id"],
        values=["running_sum_sqft_share", "running_avg_sqft_share"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
