from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium", "id")

recent_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

sequenced = (
    recent_policys
    .withColumn("rows_in_product", F.count(F.lit(1)).over(w_product))
    .withColumn("premium_seq", F.row_number().over(w_product_holder_id_by_premium))
)

leaders = sequenced.filter(F.col("premium_seq") <= 2)

annotated = (
    leaders
    .withColumn("product_ref_2", F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")))
)

result = (
    annotated
    .groupingSets(
        [["holder_id", "premium_seq", "product"], ["holder_id"], ["premium_seq"], []],
        "holder_id", "premium_seq", "product",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("rows_in_product").alias("total_rows_in_product"),
        F.countDistinct("product").alias("distinct_products"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .select(
        "holder_id",
        "premium_seq",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "gid",
        "total_rows_in_product",
        "distinct_products",
        "distinct_holder_ids",
    )
)
