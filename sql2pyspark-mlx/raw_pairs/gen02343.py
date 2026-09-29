from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.year("ends_on") >= 2023)
        .select("id", "channel")
    )
)
