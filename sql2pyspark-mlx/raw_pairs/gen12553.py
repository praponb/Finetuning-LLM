from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy(F.col("latest_rank").desc(), "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("region").isNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat("city", F.lit("/"), "store_name").alias("city_store_name"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.city_store_name",
        "s.latest_rank",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units"),
    )
)

deduped = (
    with_last
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id").isin([7, 8])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "region",
        "opened_date",
        "latest_rank",
        "units",
        F.when(F.col("region").rlike("^(Central|West)$"), "GROUP_A").when(F.col("region").rlike("^(North|South)$"), "EXTENDED").otherwise("CORE").alias("region_zone"),
    )
)
