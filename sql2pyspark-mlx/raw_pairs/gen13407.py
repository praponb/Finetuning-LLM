from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "created_at",
        "attr_key",
        F.when(F.col("attr_key").rlike("^(weight_kg|utm_source)$"), "EXTENDED").otherwise("CORE").alias("attr_key_zone"),
    )
)
