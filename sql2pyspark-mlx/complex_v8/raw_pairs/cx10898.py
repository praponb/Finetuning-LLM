from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")

base_policys = (
    df_policies
    .filter(F.col("premium") > 2096.6)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.when(F.col("product").isin("TRAVEL", "HOME"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

filtered = (
    base_policys.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
)

stats = (
    filtered
    .groupBy("product", "product_group", "status")
    .agg(
        F.sum("premium_adj").alias("total_premium_adj"),
        F.count(F.when(~F.col("product").isin("HOME", "TRAVEL"), 1)).alias("n_product_not_in_home_travel"),
        F.max("premium_adj").alias("max_premium_adj_check"),
    )
    .filter(F.col("max_premium_adj_check") > 1660.0)
    .drop("max_premium_adj_check")
)

with_windows = (
    stats
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
    )
)

result = (
    with_windows
    .select(
        "product",
        "product_group",
        "rows_in_status",
        "n_product_not_in_home_travel",
    )
    .unpivot(
        ids=["product", "product_group"],
        values=["rows_in_status", "n_product_not_in_home_travel"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
