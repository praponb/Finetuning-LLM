from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id")
w_holder_joined_on_month_produ = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), "product").orderBy("premium", "id")
w_all_by_holder_joined_on = Window.orderBy(F.col("holder_joined_on").desc(), "id")
w_all_by_holder_joined_on_ties = Window.orderBy(F.col("holder_joined_on").desc())
w_all_by_holder_joined_on_movi = Window.orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_premium_seq_premium_quartile = Window.partitionBy("premium_seq", "premium_quartile").orderBy(F.col("product_max_premium").desc(), "id")
w_seq_no_by_distinct_holder_id = Window.partitionBy("seq_no").orderBy("distinct_holder_id_in_product", "id")
w_premium_quartile_by_premium = Window.partitionBy("premium_quartile").orderBy("premium_change")
w_premium_seq_premium_quartile_2 = Window.partitionBy("premium_seq", "premium_quartile").orderBy(F.col("product_max_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_policies
    .filter(F.col("product") == "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
    )
)

with_parent_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_trends = (
    with_parent_holder
    .select(
        "id",
        "holder_joined_on",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.row_number().over(w_holder_id_by_start_date).alias("seq_no"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
        F.ntile(4).over(w_holder_joined_on_month_produ).alias("premium_quartile"),
        (F.col("premium") - F.lag("premium").over(w_holder_joined_on_month_produ)).alias("premium_change"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.row_number().over(w_holder_joined_on_month_produ).alias("premium_seq"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_holder_joined_on)).alias("days_since_prev_start_date"),
        F.round(F.cume_dist().over(w_all_by_holder_joined_on_ties), 3).alias("holder_joined_on_cume_dist"),
        F.max("premium").over(w_all_by_holder_joined_on_movi).alias("premium_max_7"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.rank().over(w_premium_seq_premium_quartile).alias("product_max_premium_rank"),
        F.ntile(5).over(w_seq_no_by_distinct_holder_id).alias("distinct_holder_id_in_product_quintile"),
        F.dense_rank().over(w_premium_quartile_by_premium).alias("premium_change_dense_rank"),
        F.nth_value("premium_quartile", 2).over(w_premium_seq_premium_quartile_2).alias("second_premium_quartile"),
    )
)

result = (
    ranked
    .filter(F.col("premium_share_of_product") < 0.8)
    .select(
        "id",
        "holder_joined_on",
        "premium_share_of_product",
        "distinct_holder_id_in_product",
        "premium_quartile",
        "rows_in_product",
        "premium_seq",
        "holder_joined_on_cume_dist",
        "premium_max_7",
        "product_max_premium_rank",
        "distinct_holder_id_in_product_quintile",
        "premium_change_dense_rank",
        "second_premium_quartile",
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
    )
    .orderBy(F.col("id").desc(), F.col("premium_max_7").desc())
)
