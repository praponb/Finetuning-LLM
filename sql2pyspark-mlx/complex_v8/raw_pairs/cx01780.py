from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("distinct_site_ids_tier", "quality_or_unknown").orderBy(F.col("greatest_cnt_distinct_qualitys").desc(), F.col("quality").desc())
w_all_by_greatest_cnt_distinct = Window.orderBy(F.col("greatest_cnt_distinct_qualitys").desc())
w_all_by_greatest_cnt_distinct_2 = Window.orderBy(F.col("greatest_cnt_distinct_qualitys").desc(), "quality")
w_greatest_cnt_distinct_qualit = Window.partitionBy("greatest_cnt_distinct_qualitys", "quality_or_unknown")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 20.4)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

rollup_base = (
    cleaned_readings
    .filter(F.col("kwh") <= 104.8)
    .groupBy("quality")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

annotated = (
    rollup_base
    .filter(~F.col("quality").isin("OK", "EST"))
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.greatest(F.col("cnt"), F.col("distinct_qualitys")).alias("greatest_cnt_distinct_qualitys"),
    )
)

annotated_2 = (
    annotated
    .select(
        "quality",
        "distinct_qualitys",
        "quality_or_unknown",
        "greatest_cnt_distinct_qualitys",
        F.when(F.col("distinct_site_ids") >= 9, "HIGH").when(F.col("distinct_site_ids") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_site_ids_tier"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

unique_rows = (
    annotated_2
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    unique_rows
    .select(
        "*",
        F.dense_rank().over(w_all_by_greatest_cnt_distinct).alias("greatest_cnt_distinct_qualitys_dense_rank"),
        F.lead("distinct_qualitys", 2, 0).over(w_all_by_greatest_cnt_distinct_2).alias("next2_distinct_qualitys"),
        F.round(F.percent_rank().over(w_all_by_greatest_cnt_distinct), 3).alias("greatest_cnt_distinct_qualitys_percent_rank"),
        F.round(F.col("distinct_qualitys") - F.avg("distinct_qualitys").over(w_greatest_cnt_distinct_qualit), 2).alias("distinct_qualitys_vs_greatest_cnt_distinct_quali"),
    )
)

result = (
    sequenced
    .select(
        "quality",
        "distinct_qualitys",
        "greatest_cnt_distinct_qualitys",
        "distinct_site_ids_tier",
        "quality_clean",
        "greatest_cnt_distinct_qualitys_dense_rank",
        "next2_distinct_qualitys",
        "greatest_cnt_distinct_qualitys_percent_rank",
        "distinct_qualitys_vs_greatest_cnt_distinct_quali",
    )
    .orderBy(F.col("greatest_cnt_distinct_qualitys").desc_nulls_first())
)
