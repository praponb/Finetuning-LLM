from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_cnt_by_cnt_ties = Window.partitionBy("product", "cnt").orderBy("cnt")
w_cnt_by_total_premium = Window.partitionBy("cnt").orderBy(F.col("total_premium").desc(), "start_date_ym", "product")
w_cnt_product_by_total_premium = Window.partitionBy("cnt", "product").orderBy("total_premium", "start_date_ym", "product")
w_total_premium_quintile_by_to = Window.partitionBy("total_premium_quintile").orderBy(F.col("total_premium_quintile").desc())
w_total_premium_quintile_by_to_2 = Window.partitionBy("total_premium_quintile").orderBy(F.col("total_premium_quintile").desc(), "start_date_ym", "product")

filtered_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
    )
)

grouped = (
    filtered_policys
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "product",
    )
    .agg(
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.count("*").alias("cnt"),
        F.min("premium").alias("min_premium"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        F.round(F.percent_rank().over(w_product_cnt_by_cnt_ties), 3).alias("cnt_percent_rank"),
        F.ntile(5).over(w_cnt_by_total_premium).alias("total_premium_quintile"),
        (F.col("min_premium") - F.lag("min_premium").over(w_cnt_product_by_total_premium)).alias("min_premium_change"),
    )
)

with_trends = (
    ranked
    .select(
        "start_date_ym",
        "product",
        "total_premium",
        "cnt",
        "min_premium",
        "cnt_percent_rank",
        "min_premium_change",
        F.round(F.cume_dist().over(w_total_premium_quintile_by_to), 3).alias("total_premium_quintile_cume_dist"),
        F.lag("min_premium", 3).over(w_total_premium_quintile_by_to_2).alias("prev3_min_premium"),
    )
)

result = (
    with_trends
    .select(
        "start_date_ym",
        "product",
        "total_premium_quintile_cume_dist",
        "total_premium",
    )
    .unpivot(
        ids=["start_date_ym", "product"],
        values=["total_premium_quintile_cume_dist", "total_premium"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
