from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("name")) == "PIM CHAIYAPORN")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.min("age").alias("min_age"),
    )
)
