from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_year_b = Window.partitionBy(F.year("commissioned_on_month")).orderBy("installed_kw", "id").rowsBetween(-2, Window.currentRow)
w_commissioned_on_month_year_b_2 = Window.partitionBy(F.year("commissioned_on_month")).orderBy("installed_kw", "id")
w_commissioned_on_month_year_b_3 = Window.partitionBy(F.year("commissioned_on_month")).orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_commissioned_on_month_year_b_4 = Window.partitionBy(F.year("commissioned_on_month")).orderBy("installed_kw")
w_commissioned_on_plus_365d_mo = Window.partitionBy(F.month("commissioned_on_plus_365d")).orderBy("installed_kw", "id")
w_commissioned_on_plus_365d_mo_2 = Window.partitionBy(F.month("commissioned_on_plus_365d"))
w_commissioned_on_plus_365d_mo_3 = Window.partitionBy(F.month("commissioned_on_plus_365d")).orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_commissioned_on_plus_365d_mo_4 = Window.partitionBy(F.month("commissioned_on_plus_365d")).orderBy("installed_kw", "id").rowsBetween(-1, -1)

valid_sites = (
    df_sites
    .filter(F.col("zone") != "Z4")
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("month", F.col("commissioned_on")).alias("commissioned_on_month"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.initcap(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

ranked = (
    valid_sites
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        "tariff_clean",
        F.round(F.avg("installed_kw").over(w_commissioned_on_month_year_b), 2).alias("installed_kw_avg_3"),
        (F.col("installed_kw") - F.lag("installed_kw").over(w_commissioned_on_month_year_b_2)).alias("installed_kw_change"),
        F.last("commissioned_on").over(w_commissioned_on_month_year_b_3).alias("last_commissioned_on"),
        F.dense_rank().over(w_commissioned_on_month_year_b_4).alias("installed_kw_dense_rank"),
    )
)

scored = (
    ranked
    .select(
        "*",
        F.date_format(F.col("commissioned_on"), "yyyy-MM").alias("commissioned_on_label"),
        (F.floor(F.col("installed_kw_change") / 250) * 250).alias("installed_kw_change_bucket"),
        F.date_add(F.col("commissioned_on"), 365).alias("commissioned_on_plus_365d"),
    )
)

top_per_group = (
    scored
    .filter(F.col("installed_kw_dense_rank") <= 3)
    .drop("installed_kw_dense_rank")
)

ranked_2 = (
    top_per_group
    .select(
        "*",
        F.lag("installed_kw_change_bucket", 2).over(w_commissioned_on_plus_365d_mo).alias("prev2_installed_kw_change_bucket"),
        F.sum("installed_kw_change").over(w_commissioned_on_plus_365d_mo_2).alias("commissioned_on_plus_365d_month_sum_installed_kw"),
        F.count(F.lit(1)).over(w_commissioned_on_plus_365d_mo_3).alias("running_count"),
        F.round(F.avg("installed_kw_change").over(w_commissioned_on_plus_365d_mo_4), 2).alias("installed_kw_change_avg_1_prior"),
    )
)

derived = (
    ranked_2
    .select(
        "id",
        "site_name",
        "installed_kw",
        "installed_kw_change",
        "commissioned_on_label",
        "prev2_installed_kw_change_bucket",
        "commissioned_on_plus_365d_month_sum_installed_kw",
        F.date_format(F.col("last_commissioned_on"), "MMM yyyy").alias("last_commissioned_on_label"),
    )
)

result = (
    derived
    .select(
        "id",
        "site_name",
        "installed_kw",
        "installed_kw_change",
        "commissioned_on_label",
        "commissioned_on_plus_365d_month_sum_installed_kw",
        "last_commissioned_on_label",
        F.least(F.col("installed_kw"), F.col("installed_kw_change")).alias("least_installed_kw_installed_kw_change"),
    )
    .orderBy(F.col("last_commissioned_on_label").desc())
)
