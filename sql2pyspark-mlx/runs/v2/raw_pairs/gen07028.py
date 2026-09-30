from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.5) & F.col("rating").between(1.9, 2.3))
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)
