from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("page_upper", F.upper("page"))
    .withColumn("clean_page", F.trim("page"))
)
