from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["New York", "Berlin"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy(F.col("city_group").desc())
)
