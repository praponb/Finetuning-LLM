from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("capacity_tier").orderBy(F.col("city_len").desc(), F.col("id").desc())

prepared = (
    df_warehouses
    .filter((F.col("capacity") <= 11506) & (~F.col("city").isin("Bangkok", "Singapore")))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.when(F.col("capacity") >= 11768, "A").when(F.col("capacity") >= 4912, "B").otherwise("C").alias("capacity_tier"),
        F.length(F.trim(F.col("city"))).alias("city_len"),
    )
)

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["capacity_tier", "city_len"], ["capacity_tier"], ["city_len"]],
        "capacity_tier", "city_len",
    )
    .agg(
        F.grouping("capacity_tier").alias("is_total_capacity_tier"),
        F.round(F.avg("capacity"), 2).alias("avg_capacity"),
        F.sum(F.when(F.upper(F.trim(F.col("wh_code"))).like("B%"), 1).otherwise(0)).alias("n_upper_trim_wh_code_like_b"),
        F.array_sort(F.collect_set("wh_code")).alias("wh_code_list"),
    )
    .orderBy(
        F.col("capacity_tier").asc_nulls_last(),
        F.col("city_len").asc_nulls_last(),
    )
)
