from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month = Window.partitionBy(F.month("start_date"))
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium_distance_2_adj", "id")

cleaned_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.abs(F.col("premium") - 2945.9).alias("premium_distance"),
        F.abs(F.col("premium") - 3532.7).alias("premium_distance_2"),
        F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 2033.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

enriched = (
    cleaned_policys
    .withColumn("premium_distance_2_adj", F.round(F.col("premium_distance_2") * 0.9, 2))
)

holder_id_stats = (
    enriched
    .groupBy("holder_id")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

vs_group = (
    enriched.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

ranked = (
    vs_group
    .select(
        "*",
        F.max("premium").over(w_start_date_month).alias("start_date_month_max_premium"),
        F.rank().over(w_product_holder_id_by_premium).alias("premium_distance_2_adj_rank"),
        F.round(F.col("median_premium") - F.avg("median_premium").over(w_start_date_month), 2).alias("median_premium_vs_start_date_month_avg"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
    )
)

result = (
    ranked
    .select("id", "premium_distance_2_adj_rank", "rows_in_start_date_month")
    .unpivot(
        ids=["id"],
        values=["premium_distance_2_adj_rank", "rows_in_start_date_month"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
