from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product_group").orderBy(F.col("start_date").desc(), F.col("id").desc())

prepared_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.when(F.col("product").isin("AUTO", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

latest_per_key = (
    prepared_policys
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined_holder = (
    latest_per_key.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.segment").alias("holder_segment"), "p.holder_name")
)

result = (
    joined_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "product_prefix",
        "status_ref",
        "holder_segment",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
    .orderBy("end_date")
)
