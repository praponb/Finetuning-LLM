from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_plus_12m_year_sta = Window.partitionBy(F.year("start_date_plus_12m"), "status")
w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")

valid_policys = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.abs(F.col("premium") - 2096.6).alias("premium_distance"),
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
    )
)

with_windows = (
    valid_policys
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_plus_12m_year_sta), F.lit(0)), 4).alias("premium_share_of_start_date_plus_12m_year_status"),
        F.sum("premium_distance").over(w_holder_id_product_by_premium).alias("running_premium_distance"),
        F.ntile(3).over(w_holder_id_by_start_date).alias("start_date_tercile"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status",
        "premium_distance",
        "start_date_plus_12m",
        "running_premium_distance",
        "start_date_tercile",
        F.abs(F.col("running_premium_distance") - 17500.0).alias("running_premium_distance_distance"),
        F.add_months(F.col("start_date_plus_12m"), 1).alias("start_date_plus_12m_plus_1m"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

product_stats = (
    scored
    .groupBy("product")
    .agg(F.avg("running_premium_distance").alias("avg_running_premium_distance"))
)

vs_group = (
    scored.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.running_premium_distance") > F.col("g.avg_running_premium_distance"))
    .select(
        "s.*",
        "g.avg_running_premium_distance",
        F.round(F.col("s.running_premium_distance") - F.col("g.avg_running_premium_distance"), 2).alias("running_premium_distance_minus_avg"),
    )
)

result = (
    vs_group
    .groupBy("status", "start_date")
    .pivot("product", ["AUTO", "LIFE", "HOME"])
    .agg(F.min("premium_distance").alias("min_v"), F.sum("premium_distance").alias("sum_v"))
)
