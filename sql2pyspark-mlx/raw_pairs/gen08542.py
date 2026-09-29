from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("id", "country", "rating", F.upper("country").alias("country_upper"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

latest = (
    with_latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_latest_suppliers = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.brand"))
)

remaining_keys = (
    with_latest_suppliers
    .select("country", "id")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "London")
        .select("country", "id")
    )
)

reconciled = (
    with_latest_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy("brand")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
)

result = totals.select("brand", "avg_price", "distinct_brands", "num_brands")
