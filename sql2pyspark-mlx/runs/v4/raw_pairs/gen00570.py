from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)
