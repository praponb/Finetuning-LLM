from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category_group").orderBy(F.col("weight_kg").desc(), F.col("id").desc(), F.col("label").desc())
w_is_active_category_group_by = Window.partitionBy("is_active", "category_group").orderBy("weight_kg", "id", "label").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_active_category_group_by_2 = Window.partitionBy("is_active", "category_group").orderBy("weight_kg", "id", "label").rowsBetween(-6, Window.currentRow)

prepared = (
    df_skus
    .filter((F.col("unit_cost") >= 164.5) & (F.col("weight_kg") < 14.3))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("unit_cost") >= 306.4, "HIGH").when(F.col("unit_cost") >= 68.0, "MEDIUM").otherwise("LOW").alias("unit_cost_tier"),
        F.when(F.col("category").isin("Food", "Apparel"), "group_a").when(F.col("category") == "Electronics", "group_b").otherwise("other").alias("category_group"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.qty") >= 168) & (F.col("st.operator") == "op_ann"), "left_anti")
)

flattened = (
    qualified
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        "unit_cost_tier",
        "category_group",
        F.explode("labels").alias("label"),
    )
)

deduped = (
    flattened
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    deduped
    .select(
        "id",
        "sku_code",
        "category",
        "is_active",
        "category_group",
        "label",
        F.nth_value("weight_kg", 2).over(w_is_active_category_group_by).alias("second_weight_kg"),
        F.sum("weight_kg").over(w_is_active_category_group_by_2).alias("weight_kg_sum_7"),
    )
)

result = (
    sequenced
    .filter(F.col("category_group").isin("other", "group_b"))
    .select(
        "id",
        "sku_code",
        "category",
        "is_active",
        "category_group",
        "label",
        F.coalesce(F.col("second_weight_kg"), F.lit(0.0)).alias("second_weight_kg_filled"),
        (F.col("weight_kg_sum_7") - F.col("second_weight_kg")).alias("weight_kg_sum_7_minus_second_weight_kg"),
    )
    .orderBy("id", "label")
    .limit(20)
)
